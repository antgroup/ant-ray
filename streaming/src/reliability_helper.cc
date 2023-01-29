#include "reliability_helper.h"

#include <boost/asio/thread_pool.hpp>
namespace ray {
namespace streaming {

constexpr uint32_t ExactlySameHelper::kMaxThreadPoolNum;

std::shared_ptr<ReliabilityHelper> ReliabilityHelperFactory::GenReliabilityHelper(
    StreamingConfig &config, StreamingBarrierHelper &barrier_helper, DataWriter *writer,
    DataReader *reader) {
  if (config.IsExactlySame()) {
    return std::make_shared<ExactlySameHelper>(config, barrier_helper, writer, reader);
  } else if (config.IsExactlyOnce()) {
    return std::make_shared<ExactlyOnceHelper>(config, barrier_helper, writer, reader);
  } else {
    return std::make_shared<AtLeastOnceHelper>(config, barrier_helper, writer, reader);
  }
}

ReliabilityHelper::ReliabilityHelper(StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : config_(config),
      barrier_helper_(barrier_helper),
      writer_(writer),
      reader_(reader) {}

void ReliabilityHelper::Reload() {}

bool ReliabilityHelper::StoreBundleMeta(ProducerChannelInfo &channel_info,
                                        StreamingMessageBundlePtr &bundle_ptr,
                                        bool is_replay) {
  return false;
}

bool ReliabilityHelper::FilterMessage(ProducerChannelInfo &channel_info,
                                      const uint8_t *data,
                                      StreamingMessageType message_type,
                                      uint64_t *write_message_id) {
  bool is_filtered = false;
  uint64_t &message_id = channel_info.current_message_id;
  uint64_t last_msg_id = channel_info.message_last_commit_id;

  if (StreamingMessageType::Barrier == message_type) {
    is_filtered = message_id < last_msg_id;
    // NOTE(lingxuan.zlx): Marking first barrier without any message before in id 1.
    // Downstream will subscribe message in id index from consumed message id of last
    // checkpoint, so the first subcribed id will be 1. According above described,
    // downstream will drop this barrier if its message id is 0. It's to say we can not
    // guarantee downstream vertices will be starting before upstreams, but partial
    // barrier/global barrier may be critical for checkpoint/rescaling.
    if (!is_filtered && 0 == message_id) {
      message_id = 1;
    }
  } else {
    message_id++;
    // Message last commit id is the last item in queue or restore from queue.
    // It skip directly since message id is less or equal than current commit id.
    is_filtered = message_id <= last_msg_id && !config_.IsAtLeastOnce();
  }
  *write_message_id = message_id;

  return is_filtered;
}

void ReliabilityHelper::CleanupCheckpoint(ProducerChannelInfo &channel_info,
                                          uint64_t barrier_id) {}

StreamingStatus ReliabilityHelper::InitChannelMerger(uint32_t timeout) {
  return reader_->InitChannelMerger(timeout);
}

StreamingStatus ReliabilityHelper::StashNextMessage(
    std::shared_ptr<StreamingReaderBundle> &message, uint32_t timeout_ms) {
  if (reader_->last_fetched_queue_item_ == nullptr) {
    STREAMING_LOG(INFO) << "Getting msg from merger.";
    message = reader_->reader_merger_->top();
    return StreamingStatus::OK;
  }

  // push new message into priority queue
  std::shared_ptr<StreamingReaderBundle> new_msg =
      std::make_shared<StreamingReaderBundle>();
  auto &channel_info =
      reader_->channel_info_map_[reader_->last_fetched_queue_item_->from];
  RETURN_IF_NOT_OK(
      reader_->GetMessageFromChannel(channel_info, new_msg, timeout_ms, timeout_ms));
  reader_->reader_merger_->pop();
  new_msg->last_barrier_id = channel_info.barrier_id;
  new_msg->last_udc_barrier_id = channel_info.udc_msg_barrier_id;
  new_msg->timestamp_push = current_sys_time_ms();
  reader_->reader_merger_->push(new_msg);
  channel_info.last_queue_item_delay =
      new_msg->meta->GetMessageBundleTs() -
      reader_->last_fetched_queue_item_->meta->GetMessageBundleTs();
  message = reader_->reader_merger_->top();
  return StreamingStatus::OK;
}

StreamingStatus ReliabilityHelper::HandleNoValidItem(ConsumerChannelInfo &channel_info) {
  STREAMING_LOG(DEBUG) << "[Reader] Queue " << channel_info.channel_id
                       << " get item timeout, resend notify "
                       << channel_info.current_message_id;
  reader_->NotifyConsumedItem(channel_info, channel_info.current_message_id);
  return StreamingStatus::OK;
}

AtLeastOnceHelper::AtLeastOnceHelper(StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : ReliabilityHelper(config, barrier_helper, writer, reader) {}

StreamingStatus AtLeastOnceHelper::InitChannelMerger(uint32_t timeout) {
  // No merge in AT_LEAST_ONCE
  return StreamingStatus::OK;
}

StreamingStatus AtLeastOnceHelper::StashNextMessage(
    std::shared_ptr<StreamingReaderBundle> &message, uint32_t timeout_ms) {
  // read all channels in round-robin order
  int64_t start_time = current_sys_time_ms();
  message = std::make_shared<StreamingReaderBundle>();
  reader_->AdvanceChannelInfoMapIter();
  auto start_iter = reader_->channel_info_map_iter_;
  while (true) {
    auto &channel_info = reader_->channel_info_map_iter_->second;
    StreamingStatus st =
        reader_->GetMessageFromChannel(channel_info, message, timeout_ms, 0);
    if (StreamingStatus::Invalid == st || StreamingStatus::GetBundleTimeOut == st) {
      reader_->AdvanceChannelInfoMapIter();
    } else {
      if (StreamingStatus::OK != st) {
        STREAMING_LOG(WARNING) << "StashNextMessage unexpected status: " << st;
      }
      break;
    }
    // If there is no items in all channels, sleep a while to reduce cpu.
    if (start_iter == reader_->channel_info_map_iter_) {
      std::this_thread::sleep_for(
          std::chrono::microseconds(config_.GetStreamingReaderRoundRobinSleepTimeUs()));
      if (current_sys_time_ms() - start_time >= timeout_ms) {
        return StreamingStatus::GetBundleTimeOut;
      }
    }
  }

  if (reader_->runtime_context_->transfer_state_.IsInterrupted()) {
    return StreamingStatus::Interrupted;
  }

  auto &channel_info = reader_->channel_info_map_iter_->second;
  message->last_barrier_id = channel_info.barrier_id;
  message->timestamp_push = current_sys_time_ms();
  if (reader_->last_fetched_queue_item_) {
    channel_info.last_queue_item_delay =
        message->meta->GetMessageBundleTs() -
        reader_->last_fetched_queue_item_->meta->GetMessageBundleTs();
  }
  return StreamingStatus::OK;
}

StreamingStatus AtLeastOnceHelper::HandleNoValidItem(ConsumerChannelInfo &channel_info) {
  if (current_sys_time_ms() - channel_info.resend_notify_timer >
      StreamingConfig::STREAMING_RESEND_NOTIFY_MAX_INTERVAL) {
    STREAMING_LOG(INFO) << "[Reader] Queue " << channel_info.channel_id
                        << " get item timeout, resend notify "
                        << channel_info.current_message_id;
    reader_->NotifyConsumedItem(channel_info, channel_info.current_message_id);
    channel_info.resend_notify_timer = current_sys_time_ms();
  }
  return StreamingStatus::Invalid;
}

ExactlyOnceHelper::ExactlyOnceHelper(StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : ReliabilityHelper(config, barrier_helper, writer, reader) {}

bool ExactlyOnceHelper::FilterMessage(ProducerChannelInfo &channel_info,
                                      const uint8_t *data,
                                      StreamingMessageType message_type,
                                      uint64_t *write_message_id) {
  bool is_filtered = ReliabilityHelper::FilterMessage(channel_info, data, message_type,
                                                      write_message_id);
  if (is_filtered && StreamingMessageType::Barrier == message_type &&
      StreamingRole::SOURCE == config_.GetStreamingRole()) {
    *write_message_id = channel_info.message_last_commit_id;
    // Do not skip source barrier when it's reconstructing from downstream.
    is_filtered = false;
    STREAMING_LOG(INFO) << "append barrier to buffer ring " << *write_message_id
                        << ", last commit id " << channel_info.message_last_commit_id;
  }
  return is_filtered;
}

ExactlySameHelper::ExactlySameHelper(StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : ReliabilityHelper(config, barrier_helper, writer, reader),
      persistence_helper_(
          new StreamingMetaPersistence(config_.GetStreamingPersistenceCheckpointMaxCnt(),
                                       config_.GetStreamingPersistencePath(),
                                       config_.GetStreamingRollbackCheckpointId(),
                                       config_.GetStreamingPersistenceClusterName())) {
  STREAMING_LOG(INFO) << "persistence path => " << config_.GetStreamingPersistencePath();
}

void ExactlySameHelper::Reload() {
  uint32_t queue_num = writer_->output_queue_ids_.size();
  // Thread pool are employed to speed up reload meta info from thridparty storage.
  boost::asio::thread_pool thread_pool(queue_num > kMaxThreadPoolNum ? kMaxThreadPoolNum
                                                                     : queue_num);
  std::unordered_map<ObjectID, std::vector<StreamingMessageBundleMetaPtr>>
      reload_meta_map;
  std::unordered_map<ObjectID, StreamingMessageList> reload_stashed_message_list;
  for (auto &channel_item : writer_->channel_info_map_) {
    auto &channel_info = channel_item.second;
    const auto &queue_id = channel_info.channel_id;
    reload_meta_map[queue_id] = std::vector<StreamingMessageBundleMetaPtr>();
    reload_stashed_message_list[queue_id] = StreamingMessageList();

    boost::asio::post(thread_pool, std::bind(&ExactlySameHelper::ReloadByQueueId, this,
                                             std::ref(channel_info),
                                             std::ref(reload_meta_map[queue_id])));
  }
  thread_pool.join();
  // Replay all data according to reloaded meta info.
  do {
    auto reload_meta_it = reload_meta_map.begin();
    while (writer_->runtime_context_->GetTransferState().IsRunning() &&
           reload_meta_it != reload_meta_map.end()) {
      if (reload_meta_it->second.size()) {
        auto &meta_ptr = reload_meta_it->second.back();
        STREAMING_LOG(DEBUG) << "q id => " << reload_meta_it->first << ", meta info =>"
                             << meta_ptr->ToString();
        auto &reload_message_list = reload_stashed_message_list[reload_meta_it->first];
        auto &channel_info = writer_->channel_info_map_[reload_meta_it->first];
        this->ReplayByQueueId(channel_info, reload_message_list, reload_meta_it->second);
        reload_meta_it++;
      } else {
        reload_meta_it = reload_meta_map.erase(reload_meta_it);
      }
    }

  } while (writer_->runtime_context_->GetTransferState().IsRunning() &&
           reload_meta_map.size());

  // For thread safely join in failover case : upstream close firstly, so downstream can't
  // reload neither.
  if (!writer_->runtime_context_->GetTransferState().IsRunning()) {
    STREAMING_LOG(WARNING) << "interrupt in streaming reload, "
                           << " channel state => "
                           << writer_->runtime_context_->GetTransferState();
  }
}

void ExactlySameHelper::ReloadByQueueId(
    ProducerChannelInfo &channel_info,
    std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec) {
  auto &q_id = channel_info.channel_id;
  uint64_t max_checkpoint_id_in_queue = 0;
  barrier_helper_.GetCurrentMaxCheckpointIdInQueue(q_id, max_checkpoint_id_in_queue);
  persistence_helper_->Load(max_checkpoint_id_in_queue, bundle_meta_vec, q_id);
  STREAMING_LOG(INFO) << "[Writer] [Reload]"
                      << " q id = > " << q_id << ", max checkpoint id in queue => "
                      << max_checkpoint_id_in_queue;

  if (!bundle_meta_vec.size()) {
    STREAMING_LOG(WARNING) << "no meta data or no file to reload " << q_id;
    return;
  }

  auto bundle_it_end = std::remove_if(bundle_meta_vec.begin(), bundle_meta_vec.end(),
                                      [&](StreamingMessageBundleMetaPtr &meta_ptr) {
                                        return channel_info.message_last_commit_id >=
                                               meta_ptr->GetLastMessageId();
                                      });
  bundle_meta_vec.erase(bundle_it_end, bundle_meta_vec.end());
  // We use back and pop back to replay all data, so reverse first.
  std::reverse(bundle_meta_vec.begin(), bundle_meta_vec.end());
  STREAMING_LOG(INFO) << "Reload meta size => " << bundle_meta_vec.size();
}

void ExactlySameHelper::ReplayByQueueId(
    ProducerChannelInfo &channel_info, StreamingMessageList &message_list_fusion,
    std::vector<StreamingMessageBundleMetaPtr> &meta_vec) {
  STREAMING_CHECK(meta_vec.size());
  auto &meta_ptr = meta_vec.back();
  StreamingStatus status = StreamingStatus::TailStatus;
  auto &q_id = channel_info.channel_id;

  switch (meta_ptr->GetBundleType()) {
  case StreamingMessageBundleType::Empty:
    // Empty message bundle will be stored in channel_info if queue is full.
    if (channel_info.bundle) {
      status = writer_->WriteBundleToChannel(channel_info);
    } else {
      status = writer_->WriteEmptyMessage(channel_info, meta_ptr);
    }
    break;
  case StreamingMessageBundleType::Barrier:
    // Ignore barrier bundle if this's source
    if (config_.GetStreamingRole() == StreamingRole::SOURCE) {
      status = StreamingStatus::OK;
      break;
    }
  case StreamingMessageBundleType::Bundle:
    uint64_t buffer_remain = 0;
    if (channel_info.bundle ||
        writer_->CollectFromRingBuffer(channel_info, buffer_remain, &message_list_fusion,
                                       meta_ptr)) {
      status = writer_->WriteBundleToChannel(channel_info);
      STREAMING_LOG(DEBUG) << "[Replay] WriteBundleToChannel done, status=" << status;
    } else {
      STREAMING_LOG(DEBUG) << " empty or no needed size in ring buffer";
    }
    break;
  }
  if (StreamingStatus::OK == status) {
    meta_vec.pop_back();
    message_list_fusion.message_list.clear();
    message_list_fusion.bundle_size = 0;
  } else {
    STREAMING_LOG(DEBUG) << "[Writer] [Realod] q id => " << q_id
                         << ", no data from meta vec was sent"
                         << ", status => " << static_cast<uint32_t>(status);
  }
}

bool ExactlySameHelper::StoreBundleMeta(ProducerChannelInfo &channel_info,
                                        StreamingMessageBundlePtr &bundle_ptr,
                                        bool is_replay) {
  StreamingMessageBundleMetaPtr transient_bundle_meta(
      new StreamingMessageBundleMeta(bundle_ptr.get()));

  if (transient_bundle_meta->IsBarrier()) {
    uint64_t global_barrier_id = 0;
    StreamingStatus status = barrier_helper_.GetBarrierIdByLastMessageId(
        channel_info.channel_id, transient_bundle_meta->GetLastMessageId(),
        global_barrier_id);
    // Get current max checkpoint id no matter removing
    uint64_t current_max_checkpoint_id = 0;
    if (StreamingStatus::OK == status &&
        StreamingStatus::OK == barrier_helper_.GetCheckpointIdByBarrierId(
                                   global_barrier_id, current_max_checkpoint_id)) {
      STREAMING_LOG(INFO)
          << "[Writer] [Barrier] [Persistence] update current max checkpoint id "
          << ", q id => " << channel_info.channel_id << ", current max checkpoint id => "
          << current_max_checkpoint_id;
      barrier_helper_.SetCurrentMaxCheckpointIdInQueue(channel_info.channel_id,
                                                       current_max_checkpoint_id);
    }

    STREAMING_LOG(INFO) << "[Writer] [Barrier] [Persistence], q id => "
                        << channel_info.channel_id << ", global barrier id => "
                        << global_barrier_id << ", current max checkpoint id => "
                        << current_max_checkpoint_id << ", last message id => "
                        << transient_bundle_meta->GetLastMessageId();
  }
  // Only new data will be stored.
  if (!is_replay) {
    uint64_t max_checkpoint_id_in_queue = 0;
    barrier_helper_.GetCurrentMaxCheckpointIdInQueue(channel_info.channel_id,
                                                     max_checkpoint_id_in_queue);
    STREAMING_LOG(DEBUG) << "store max checkpoint id in queue => "
                         << max_checkpoint_id_in_queue;
    persistence_helper_->Store(max_checkpoint_id_in_queue, transient_bundle_meta,
                               channel_info.channel_id);
    return true;
  }
  return false;
}

void ExactlySameHelper::CleanupCheckpoint(ProducerChannelInfo &channel_info,
                                          uint64_t barrier_id) {
  persistence_helper_->Clear(channel_info.channel_id, barrier_id);
}
}  // namespace streaming
}  // namespace ray
