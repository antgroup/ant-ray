#include "data_reader.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

#include "channel/factory.h"
#include "config.h"
#include "message_bundle.h"
#include "reliability_helper.h"

namespace ray {
namespace streaming {

const uint32_t DataReader::kReadItemTimeout = 1000;

void DataReader::Init(const std::vector<ObjectID> &input_ids,
                      const std::vector<StreamingQueueInitialParameter> &init_params,
                      const std::vector<uint64_t> &streaming_msg_ids,
                      int64_t timer_interval,
                      std::vector<TransferCreationStatus> &creation_status) {
  Init(input_ids, init_params, timer_interval);
  for (size_t i = 0; i < input_ids.size(); ++i) {
    auto &q_id = input_ids[i];
    last_message_id_[q_id] = streaming_msg_ids[i];
    initial_message_id_[q_id] = streaming_msg_ids[i];
    channel_info_map_[q_id].current_message_id = streaming_msg_ids[i];
    channel_info_map_[q_id].barrier_id =
        runtime_context_->config_.GetStreamingRollbackCheckpointId();
    channel_info_map_[q_id].partial_barrier_id = 0;
    channel_info_map_[q_id].parameter = init_params[i];
  }

  // Push all channel id to upstream barrier collection, so newborn worker
  // doesn't discard barrier.
  rescale_context_.UpdateRescaleTargetSet(input_ids);
  InitChannel(creation_status);
  runtime_context_->InitMetricsReporter();
  runtime_context_->EnableTimer(std::bind(&DataReader::ReportTimer, this));
}

void DataReader::Init(const std::vector<ObjectID> &input_ids,
                      const std::vector<StreamingQueueInitialParameter> &init_params,
                      int64_t timer_interval, bool is_rescale) {
  if (!is_rescale) {
    STREAMING_LOG(INFO) << input_ids.size() << " queue to init"
                        << ", timer interval " << timer_interval << ", log level="
                        << runtime_context_->config_.GetStreamingLogLevel();

    transfer_config_->Set(ConfigEnum::QUEUE_ID_VECTOR, input_ids);

    last_fetched_queue_item_.reset();
    reader_cache_item_memory_.reset();
    timer_interval_ = timer_interval;
    input_queue_ids_ = input_ids;
    last_bundle_ts_ = current_sys_time_ms();
    // Init step updater
    switch (runtime_context_->config_.GetStepUpdater()) {
    case StepUpdater::AVERAGE:
      step_udpater = std::make_shared<StreamingAverageStep>();
      break;
    case StepUpdater::DEFAULT:
    default:
      step_udpater = std::make_shared<StreamingDefaultStep>();
    }
    // Consumed step is suggested to be half of writer step, which means reader won't
    // waiting for
    // bundle from upstream.
    step_udpater->SetMaxStep(runtime_context_->config_.GetStreamingWriterConsumedStep() /
                             2);
    step_udpater->InitStep(runtime_context_->config_.GetStreamingReaderConsumedStep());

    if (runtime_context_->config_.GetQueueType() == TransferQueueType::STREAMING_QUEUE &&
        !queue_handler_) {
      queue_handler_ = std::make_shared<DownstreamQueueMessageHandler>(
          runtime_context_->config_.GetPlasmaSocketPath(), TransportType::DIRECTCALL,
          runtime_context_->config_.GetCollocateEnable());
      queue_handler_->Start();
    }
  } else {
    std::copy(input_ids.begin(), input_ids.end(), std::back_inserter(input_queue_ids_));
  }

  for (size_t i = 0; i < input_ids.size(); ++i) {
    auto &q_id = input_ids[i];
    STREAMING_LOG(INFO) << "[Reader] Init queue id: " << q_id;
    auto &channel_info = channel_info_map_[q_id];
    channel_info.channel_id = q_id;
    channel_info.last_queue_item_delay = 0;
    channel_info.last_queue_target_diff = 0;
    channel_info.get_queue_item_times = 0;
    channel_info.notify_cnt = 0;
    channel_info.max_notified_msg_id = 0;
    channel_info.parameter = init_params[i];
    channel_info.last_queue_item_read_latency = 0;
    channel_info.last_queue_item_read_latency_large_than_10ms = 0;
    channel_info.last_queue_item_read_latency_large_than_5ms = 0;
    channel_info.last_bundle_merger_latency = 0;
    channel_info.last_bundle_merger_latency_large_than_10ms = 0;
    channel_info.last_bundle_merger_latency_large_than_5ms = 0;
    channel_info.resend_notify_timer = 0;
  }

  target_global_barrier_cnt_ = GetTargetBarrierCount(input_queue_ids_);
  // sort it once, only making heap will be carried after that.
  sort(input_queue_ids_.begin(), input_queue_ids_.end(),
       [](const ObjectID &a, const ObjectID &b) { return a.Hash() < b.Hash(); });
  std::copy(input_ids.begin(), input_ids.end(), std::back_inserter(unready_queue_ids_));

  reliability_helper_ = ReliabilityHelperFactory::GenReliabilityHelper(
      runtime_context_->config_, barrier_helper_, nullptr, this);

  if (runtime_context_->config_.GetHttpProfilerEnable()) {
    HttpProfiler::GetHttpProfilerInstance()->RegisterDataReader(this);
  }
}

StreamingStatus DataReader::InitChannel(
    std::vector<TransferCreationStatus> &creation_status) {
  STREAMING_LOG(INFO) << "[Reader] Getting queues. total queue num "
                      << input_queue_ids_.size() << ", unready queue num => "
                      << unready_queue_ids_.size() << ", channel info map size => "
                      << channel_info_map_.size() << ", channel map size => "
                      << channel_map_.size();
  // It's deadlock since this thread holds lock that's needed for upstream operator
  // when subscribing queue by local scheduler

  std::vector<ObjectID> abnormal_queues;
  // NOTE(lingxuan.zlx) Only create channel and push it to channel map, no pull data
  // here for unblocking worker rollback. Consumer channel will be retried in get bundle
  // function if it's timeout at beginning, so we will skip this duplicated
  // creation operation for constructing channel from factory.
  for (const auto &input_channel : unready_queue_ids_) {
    auto it = channel_map_.find(input_channel);
    if (it != channel_map_.end()) {
      STREAMING_LOG(INFO) << "Channel id " << input_channel
                          << " has been already initialized.";
      creation_status.push_back(TransferCreationStatus::PullOk);
      continue;
    }
    auto &channel_info = channel_info_map_[input_channel];
    auto consumer_channel = ChannelFactory::CreateConsumerChannel(
        runtime_context_->config_.GetQueueType(), transfer_config_, channel_info,
        queue_handler_);
    TransferCreationStatus status = consumer_channel->CreateTransferChannel();
    channel_map_.emplace(input_channel, consumer_channel);
    creation_status.push_back(status);
    if (TransferCreationStatus::DataLost == status ||
        TransferCreationStatus::Failed == status ||
        TransferCreationStatus::Timeout == status) {
      abnormal_queues.push_back(channel_info.channel_id);
      STREAMING_LOG(ERROR) << "Get queue failed, id=" << input_channel
                           << ", status=" << status;
    }
  }

  runtime_context_->transfer_state_.SetRunning();
  STREAMING_LOG(INFO) << "[Reader] Reader construction done!";
  return StreamingStatus::OK;
}

StreamingStatus DataReader::InitChannelMerger(uint32_t timeout_ms) {
  STREAMING_LOG(INFO) << "[Reader] Initializing queue merger.";
  StreamingReaderMsgPtrComparator comparator(
      runtime_context_->config_.GetReliabilityLevel());

  // Init reader merger when it's first created
  if (!reader_merger_) {
    reader_merger_.reset(new PriorityQueue<std::shared_ptr<StreamingReaderBundle>,
                                           StreamingReaderMsgPtrComparator, ObjectID>(
        comparator,
        [](const std::shared_ptr<StreamingReaderBundle> &item) { return item->from; }));
  }

  // An old item in merger vector must be evicted before new queue item has been
  // pushed.
  if (!unready_queue_ids_.empty() && last_fetched_queue_item_) {
    STREAMING_LOG(INFO) << "pop old item from => " << last_fetched_queue_item_->from;
    std::shared_ptr<StreamingReaderBundle> message;
    RETURN_IF_NOT_OK(reliability_helper_->StashNextMessage(message, timeout_ms));
    last_fetched_queue_item_.reset();
  }
  // heap initialization
  std::vector<ObjectID> unready_queue_ids_stashed;
  for (auto &input_queue : unready_queue_ids_) {
    std::shared_ptr<StreamingReaderBundle> msg =
        std::make_shared<StreamingReaderBundle>();
    auto status = GetMessageFromChannel(channel_info_map_[input_queue], msg, timeout_ms,
                                        timeout_ms);
    if (StreamingStatus::OK != status) {
      STREAMING_LOG(INFO)
          << "[Reader] initializing merge, get message from channel timeout, "
          << input_queue << ", status => " << static_cast<uint32_t>(status);
      unready_queue_ids_stashed.push_back(input_queue);
      continue;
    }
    channel_info_map_[msg->from].current_message_id = msg->meta->GetLastMessageId();
    reader_merger_->push(msg);
  }
  if (unready_queue_ids_stashed.empty()) {
    STREAMING_LOG(INFO) << "[Reader] Initializing merger done.";
  } else {
    STREAMING_LOG(INFO) << "[Reader] Initializing merger unfinished.";
    unready_queue_ids_ = unready_queue_ids_stashed;
    return StreamingStatus::GetBundleTimeOut;
  }
  return StreamingStatus::OK;
}

bool DataReader::IsValidBundle(std::shared_ptr<StreamingReaderBundle> &message) {
  BundleCheckStatus status = CheckBundle(message);
  STREAMING_LOG(DEBUG) << "CheckBundle, result=" << status << " from " << message->from
                       << ", last_msg_id=" << last_message_id_[message->from]
                       << ", end_msg_id: " << message->meta->GetLastMessageId()
                       << ", bundle_id= " << message->bundle_id;

  if (status == BundleCheckStatus::OkBundle) {
    return true;
  } else {
    STREAMING_LOG(INFO) << "Invalid bundle, result=" << status << " from "
                        << message->from
                        << ", last_msg_id=" << last_message_id_[message->from]
                        << ", end_msg_id: " << message->meta->GetLastMessageId()
                        << ", bundle_id= " << message->bundle_id;
  }

  if (status == BundleCheckStatus::BundleToBeSplit &&
      !runtime_context_->config_.IsAtLeastOnce()) {
    this->SplitBundle(message, last_message_id_[message->from]);
  }
  if (status == BundleCheckStatus::BundleToBeThrown) {
    // Do not filter duplicated messages in at least once mode.
    // NOTE(lingxuan.zlx) this bundle could be be thrown becasue `start_msg_id` and
    // `end_msg_id` are both bigger than last message id of previous bundle.
    // The following condtion means only :
    // 1. In at-least-once,
    // 2. Message id is less than expected.
    // So this bundle can be provided for operator level.
    if (!runtime_context_->config_.IsAtLeastOnce() ||
        message->message_id > last_message_id_[message->from]) {
      // Upstream might send a lot of invalid bundles and these bundles are out of range
      // to downstream expected. Meanwhile if and only if the last message is barrier,
      // the barrier message actually will not be cleared until next expected bundle
      // arriving. For this reason, to avoid duplicated consumed for same barrier
      // message if it should be dropped out.
      if (message->meta->IsBarrier()) {
        STREAMING_LOG(WARNING) << "Throw barrier, msg_id "
                               << message->meta->GetLastMessageId()
                               << ", from channel id " << message->from << ", bundle id "
                               << message->bundle_id;
      }
      message->ResetBuffer();
      return false;
    }
  }
  return true;
}

StreamingStatus DataReader::GetMessageFromChannel(
    ConsumerChannelInfo &channel_info, std::shared_ptr<StreamingReaderBundle> &message,
    uint32_t timeout_ms, uint32_t wait_time_ms) {
  auto &qid = channel_info.channel_id;
  message->from = qid;
  message->cyclic = channel_info.parameter.cyclic;
  last_read_q_id_ = qid;

  bool is_valid_bundle = false;
  int64_t start_time = current_sys_time_ms();
  STREAMING_LOG(DEBUG) << "GetMessageFromChannel, timeout_ms=" << timeout_ms
                       << ", wait_time_ms=" << wait_time_ms;
  while (runtime_context_->transfer_state_.IsRunning() && !is_valid_bundle &&
         current_sys_time_ms() - start_time < timeout_ms) {
    STREAMING_LOG(DEBUG) << "[Reader] send get request queue seq id => " << qid;
    uint64_t item_received_ts;
    /// In AT_LEAST_ONCE, wait_time_ms is set to 0, means `ConsumeItemFromChannel`
    /// will return immediately if no items in queue. At the same time, `timeout_ms` is
    /// ignored.
    channel_map_[channel_info.channel_id]->ConsumeItemFromChannel(message, wait_time_ms,
                                                                  item_received_ts);

    channel_info.get_queue_item_times++;
    if (!message->MetaBuffer()) {
      RETURN_IF_NOT_OK(reliability_helper_->HandleNoValidItem(channel_info));
    } else {
      uint64_t current_time = current_sys_time_ms();
      channel_info.resend_notify_timer = current_time;
      channel_info.last_queue_item_read_latency = current_time - item_received_ts;
      if (channel_info.last_queue_item_read_latency >= 10) {
        channel_info.last_queue_item_read_latency_large_than_10ms++;
      }
      if (channel_info.last_queue_item_read_latency >= 5) {
        channel_info.last_queue_item_read_latency_large_than_5ms++;
      }
      // Note(lingxuan.zlx): To find which channel get an invalid data and
      // print channel id for debugging.
      STREAMING_CHECK(
          StreamingMessageBundleMeta::CheckBundleMagicNum(message->MetaBuffer()))
          << "Magic number invalid, from channel " << channel_info.channel_id;
      message->meta = StreamingMessageBundleMeta::FromBytes(message->MetaBuffer());

      channel_info.max_notified_msg_id =
          std::max(channel_info.max_notified_msg_id, message->message_id);
      // filter message when msg_id doesn't match.
      // reader will filter message only when using streaming queue and
      // non-at-least-once mode
      is_valid_bundle = IsValidBundle(message);
    }
  }
  if (runtime_context_->transfer_state_.IsInterrupted()) {
    return StreamingStatus::Interrupted;
  }

  if (!is_valid_bundle) {
    STREAMING_LOG(DEBUG) << "GetMessageFromChannel timeout, qid="
                         << channel_info.channel_id;
    return StreamingStatus::GetBundleTimeOut;
  }

  STREAMING_LOG(DEBUG) << "[Reader] received queue msg id => " << message->message_id
                       << ", queue id => " << qid;
  // NOTE(lingxuan.zlx): It must keep last message id increasing, otherwise data bundle
  // with larger message id might be skipped. For example, upstream handler sends [1, 100]
  // to this downstream worker but the other thread of upstream will receive OnPullRequest
  // and resend them [1, 100] again if first request is timeout. So normal trasporting and
  // resend transporting will be pushed with cross ordering, once last message id has been
  // updated in late message bundle, the normal new bundle can not pass the valid bundle
  // checker. Finally, normal bundle will be thrown by channel and no more new comming
  // message could be accepeted.
  if (last_message_id_[message->from] < message->meta->GetLastMessageId()) {
    last_message_id_[message->from] = message->meta->GetLastMessageId();
  }
  return StreamingStatus::OK;
}

BundleCheckStatus DataReader::CheckBundle(
    const std::shared_ptr<StreamingReaderBundle> &message) {
  uint64_t end_msg_id = message->meta->GetLastMessageId();
  uint64_t start_msg_id = message->meta->IsEmptyMsg()
                              ? end_msg_id
                              : end_msg_id - message->meta->GetMessageListSize() + 1;
  uint64_t last_msg_id = last_message_id_[message->from];
  uint64_t expected_msg_id = last_msg_id + 1;

  // All messages' ID is larger than expected
  if (start_msg_id > expected_msg_id) {
    return BundleCheckStatus::BundleToBeThrown;
  }

  // All messages' ID is less than expected
  // In this case all messages except barrier and empty message should not be processed.
  if (end_msg_id < expected_msg_id) {
    return end_msg_id == last_msg_id && !message->meta->IsBundle()
               ? BundleCheckStatus::OkBundle
               : BundleCheckStatus::BundleToBeThrown;
  }

  // First message ID in bundle is exactly as expected.
  // In this case all messages in this bundle should be processed if it's a message bundle
  // or barrier but not empty message.
  // NOTE(lingxuan.zlx) : we do not skip any barrier whose message id equals to expected
  // message id so that global checkpoint will be finished when nothing invalid messages
  // are in processing pipeline.
  if (start_msg_id == expected_msg_id) {
    // Accepct non-emtpy first bundle with
    if (0 == last_msg_id) {
      return !message->meta->IsEmptyMsg() ? BundleCheckStatus::OkBundle
                                          : BundleCheckStatus::BundleToBeThrown;
    } else {
      return message->meta->IsBundle() ? BundleCheckStatus::OkBundle
                                       : BundleCheckStatus::BundleToBeThrown;
    }
  }

  // Part of messages in this bundle should be processed
  return BundleCheckStatus::BundleToBeSplit;
}

void DataReader::SplitBundle(std::shared_ptr<StreamingReaderBundle> &message,
                             uint64_t last_msg_id) {
  std::list<StreamingMessagePtr> msg_list;
  StreamingMessageBundle::GetMessageListFromRawData(
      message->DataBuffer(), message->DataSize(), message->meta->GetMessageListSize(),
      msg_list);
  uint64_t bundle_size = 0;
  for (auto it = msg_list.begin(); it != msg_list.end();) {
    if ((*it)->GetMessageId() > last_msg_id) {
      bundle_size += (*it)->Size();
      it++;
    } else {
      it = msg_list.erase(it);
    }
  }
  STREAMING_LOG(DEBUG) << "Split message, from_queue_id=" << message->from
                       << ", start_msg_id=" << msg_list.front()->GetMessageId()
                       << ", end_msg_id=" << msg_list.back()->GetMessageId();
  // recreate bundle
  auto cut_msg_bundle = std::make_shared<StreamingMessageBundle>(
      msg_list, message->meta->GetMessageBundleTs(), msg_list.front()->GetMessageId(),
      msg_list.back()->GetMessageId(), StreamingMessageBundleType::Bundle, bundle_size);
  message->Realloc(cut_msg_bundle->ClassBytesSize());
  cut_msg_bundle->ToBytes(message->DataBuffer());
  message->meta = StreamingMessageBundleMeta::FromBytes(message->DataBuffer());
}

bool DataReader::BarrierAlign(std::shared_ptr<StreamingReaderBundle> &message) {
  // Currently, no barrier comes from cyclic channels.
  STREAMING_CHECK(!channel_info_map_[message->from].parameter.cyclic)
      << "unexpected barrier from cyclic channel: " << message->from;
  // Arrange barrier action when barrier is arriving.
  // Note(lingxuan.zlx) : Maybe there are some deadlock between global barrier and
  // partial barrier alignment.
  StreamingBarrierHeader barrier_header;
  StreamingMessage::GetBarrierIdFromRawData(message->DataBuffer() + kMessageHeaderSize,
                                            &barrier_header);
  uint64_t barrier_id = barrier_header.barrier_id;
  auto *barrier_align_cnt = &global_barrier_cnt_;
  auto &channel_info = channel_info_map_[message->from];
  // Target count is input vector size (global barrier)
  // or barrier collection length (partial barrier).
  uint32_t target_count = 0;

  if (barrier_header.IsGlobalBarrier()) {
    channel_info.barrier_id = barrier_header.barrier_id;
    target_count = target_global_barrier_cnt_;
  } else if (barrier_header.IsEndOfDataBarrier()) {
    barrier_align_cnt = &endofdata_barrier_cnt_;
    target_count = target_global_barrier_cnt_;
  } else if (barrier_header.IsUDCMsgBarrier()) {
    channel_info.udc_msg_barrier_id = barrier_header.barrier_id;
    barrier_align_cnt = &udc_msg_barrier_cnt_;
    target_count = target_global_barrier_cnt_;
  } else {
    if (rescale_context_.update_set_.find(message->from) ==
        rescale_context_.update_set_.end()) {
      STREAMING_LOG(ERROR) << "Invalid barrier is recieved, upstream is " << message->from
                           << ", partial barrier id is "
                           << barrier_header.partial_barrier_id;
      return false;
    }
    barrier_align_cnt = &partial_barrier_cnt_;
    channel_info.partial_barrier_id = barrier_header.partial_barrier_id;
    target_count = rescale_context_.update_set_.size();
    std::vector<ObjectID> queue_id_vec;
    queue_id_vec.insert(queue_id_vec.end(), rescale_context_.update_set_.begin(),
                        rescale_context_.update_set_.end());
    target_count = GetTargetBarrierCount(queue_id_vec);
  }
  (*barrier_align_cnt)[barrier_id]++;
  // the next message checkpoint is changed if this's barrier message.
  STREAMING_LOG(INFO) << "[Reader] [Barrier] get barrier, barrier_id=" << barrier_id
                      << ", barrier_cnt=" << (*barrier_align_cnt)[barrier_id]
                      << ", global barrier id=" << barrier_header.barrier_id
                      << ", partial barrier id=" << barrier_header.partial_barrier_id
                      << ", from q_id=" << message->from << ", barrier type ="
                      << static_cast<uint32_t>(barrier_header.barrier_type)
                      << ", target count =" << target_count;
  // notify invoker the last barrier, so that checkpoint or something related can be
  // taken right now.
  if ((*barrier_align_cnt)[barrier_id] == target_count) {
    // map can't be used in multithread (crash in report timer)
    barrier_align_cnt->erase(barrier_id);
    STREAMING_LOG(INFO)
        << "[Reader] [Barrier] last barrier received, return barrier. barrier_id = "
        << barrier_id << ", from q_id=" << message->from;
    return true;
  }
  return false;
}

void DataReader::HandleMergedBarrierMessage(
    std::shared_ptr<StreamingReaderBundle> &message, bool &is_valid_break,
    ConsumerChannelInfo &offset_info) {
  StreamingBarrierHeader barrier_header;
  StreamingMessage::GetBarrierIdFromRawData(message->DataBuffer() + kMessageHeaderSize,
                                            &barrier_header);
  // For UDC msg bundle, We also need to invoke `BarrierAlign` to update the channel
  // info.
  bool is_aligned = BarrierAlign(message);
  if (barrier_header.IsUDCMsgBarrier()) {
    // The only difference is that we need to return the msg immediately regardless of
    // whether aligned or not.
    is_valid_break = true;
    STREAMING_LOG(INFO) << "[Reader] [Bundle] Got a UDC msg bundle, bundle id => "
                        << barrier_header.barrier_id << ", were aligned? " << is_aligned;
  } else {
    // For other kind of barrier, We return the msg only when the barrier was aligned.
    if (is_aligned) {
      is_valid_break = true;
      NotifyStartLogging(offset_info.barrier_id);
    }
  }
}

StreamingStatus DataReader::GetMergedMessageBundle(
    std::shared_ptr<StreamingReaderBundle> &message, bool &is_valid_break,
    uint32_t timeout_ms, int64_t start_ts) {
  RETURN_IF_NOT_OK(reliability_helper_->StashNextMessage(message, timeout_ms));
  auto &offset_info = channel_info_map_[message->from];
  offset_info.last_bundle_merger_latency =
      current_sys_time_ms() - message->timestamp_push;
  if (offset_info.last_bundle_merger_latency >= 10) {
    offset_info.last_bundle_merger_latency_large_than_10ms++;
  }
  if (offset_info.last_bundle_merger_latency >= 5) {
    offset_info.last_bundle_merger_latency_large_than_5ms++;
  }

  uint64_t cur_queue_previous_msg_id = offset_info.current_message_id;
  STREAMING_LOG(DEBUG) << "[Reader] [Bundle] from q_id =>" << message->from << "cur => "
                       << cur_queue_previous_msg_id << ", message list size"
                       << message->meta->GetMessageListSize() << ", lst message id =>"
                       << message->meta->GetLastMessageId() << ", q msg id => "
                       << message->message_id << ", last barrier id => "
                       << message->last_barrier_id << ", data size =>"
                       << message->DataSize() << ", "
                       << message->meta->GetMessageBundleTs();
  int64_t cur_time = current_sys_time_ms();

  // NOTE(lingxuan.zlx): When the following three conditions are met, the read data will
  // be returned immediately:
  //  1. A valid bundle;
  //  2. Last barrier of current global checkpoint of this worker;
  //  3. An empty bundle and timer interval is enabled.
  if (message->meta->IsBundle()) {
    last_message_ts_ = cur_time;
    is_valid_break = true;
  } else if (message->meta->IsBarrier()) {
    last_message_ts_ = cur_time;
    HandleMergedBarrierMessage(message, is_valid_break, offset_info);
  } else if (timer_interval_ != -1 && cur_time - start_ts >= timer_interval_ &&
             message->meta->IsEmptyMsg()) {
    // sent empty message when reaching timer_interval
    last_message_ts_ = cur_time;
    is_valid_break = true;
  }

  offset_info.current_message_id = message->meta->GetLastMessageId();
  last_bundle_ts_ = message->meta->GetMessageBundleTs();
  // Diff this bundle timestamp from last item.
  last_bundle_latency_ = current_sys_time_ms() - last_bundle_ts_;

  STREAMING_LOG(DEBUG) << "[Reader] [Bundle] message type =>"
                       << static_cast<int>(message->meta->GetBundleType())
                       << " from id => " << message->from << ", queue msg id =>"
                       << message->message_id << ", message id => "
                       << message->meta->GetLastMessageId();
  last_fetched_queue_item_ = message;
  return StreamingStatus::OK;
}

StreamingStatus DataReader::GetBundle(const uint32_t timeout_ms,
                                      std::shared_ptr<StreamingReaderBundle> &message) {
  if (last_getbundle_ts_ == 0) {
    last_getbundle_latency_ = 0;
  } else {
    last_getbundle_latency_ = current_sys_time_us() - last_getbundle_ts_;
  }

  // Reader should cache the last read item in memory before this item is pushed into own
  // memory or finishs its serialization in uppper level. The data reader will notify the
  // consumed response to local queue and upstream once the next get bundle action is
  // comming.
  if (reader_cache_item_memory_) {
    NotifyConsumed(reader_cache_item_memory_);
  }

  // Get latest message until it meets two conditions :
  // 1. over timeout and 2. non-empty message has been fetched
  auto start_time = current_sys_time_ms();
  bool is_valid_break = false;
  uint32_t empty_bundle_cnt = 0;
  StreamingStatus status;
  while (!is_valid_break) {
    if (runtime_context_->transfer_state_.IsInterrupted()) {
      status = StreamingStatus::Interrupted;
      break;
    }
    // checking timeout
    auto cur_time = current_sys_time_ms();
    auto dur = cur_time - start_time;
    if (dur > timeout_ms) {
      runtime_context_->ReportMetrics("reader.empty_bundle_cnt", empty_bundle_cnt);
      status = StreamingStatus::GetBundleTimeOut;
      break;
    }
    if (!unready_queue_ids_.empty()) {
      std::vector<TransferCreationStatus> creation_status;
      status = InitChannel(creation_status);
      switch (status) {
      case StreamingStatus::InitQueueFailed:
        STREAMING_LOG(ERROR) << " init queue failed.";
        break;
      default:
        channel_info_map_iter_ = channel_info_map_.begin();
        STREAMING_LOG(INFO) << "Init reader queue in GetBundle";
      }
      if (StreamingStatus::OK != status) {
        STREAMING_LOG(WARNING) << "Channel creation status : " << status;
        break;
      }
      status = reliability_helper_->InitChannelMerger(timeout_ms);
      if (StreamingStatus::OK != status) {
        STREAMING_LOG(WARNING) << "Init channel merger status : " << status;
        break;
      }
      unready_queue_ids_.clear();
    }
    status = GetMergedMessageBundle(message, is_valid_break, timeout_ms, start_time);
    if (StreamingStatus::OK != status) {
      STREAMING_LOG(WARNING) << "Get merged messaged bundle status : " << status;
      break;
    }
    // NOTE(lingxuan.zlx): Only valid meta data parsed in message can be notified, so just
    // skip when get bundle item bundle timeout.
    if (!is_valid_break && StreamingStatus::GetBundleTimeOut != status) {
      empty_bundle_cnt++;
      NotifyConsumed(message);
    }
  }
  // Worker loads buffer from transfer layer, but this bundle may not be
  // consumed at that time(cached in directbuffer). So we should keep cache it
  // in memory until next message bundle is returned.
  if (message && message->meta && StreamingStatus::GetBundleTimeOut != status) {
    reader_cache_item_memory_ = message;
  }
  RETURN_IF_NOT_OK(status);
  last_message_latency_ += current_sys_time_ms() - start_time;
  if (message->meta->GetMessageListSize() > 0) {
    last_bundle_unit_ = message->DataSize() * 1.0 / message->meta->GetMessageListSize();
  }
  STREAMING_LOG(DEBUG) << "GetBundle OK, msg = " << *message;
  last_getbundle_ts_ = current_sys_time_us();
  return StreamingStatus::OK;
}

void DataReader::GetOffsetInfo(
    std::unordered_map<ObjectID, ConsumerChannelInfo> *&offset_map) {
  offset_map = &channel_info_map_;
  for (auto &offset_info : channel_info_map_) {
    STREAMING_LOG(INFO) << "[Reader] [GetOffsetInfo], q id " << offset_info.first
                        << ", message id => " << offset_info.second.current_message_id;
  }
}

/// NOTE(lingxuan.zlx): we assume notify consumed item must be invoked before consumed
/// bundle id had been updated, which mean downstream can remove first bundle to last
/// bundle with consumed id.
void DataReader::NotifyConsumedItem(ConsumerChannelInfo &channel_info, uint64_t offset) {
  STREAMING_LOG(DEBUG) << "NotifyConsumedItem, q_id=" << channel_info.channel_id
                       << ", offset=" << offset << " bundle id "
                       << channel_info.queue_info.consumed_bundle_id;
  channel_map_[channel_info.channel_id]->NotifyChannelConsumed(offset);
  if (offset == channel_info.queue_info.last_message_id) {
    // report metric if notify item id equals to last msg id, which means reader maybe
    // wait for next
    // message item and upstream couldn't produce continuous data blocks.
    STREAMING_LOG(DEBUG) << "notify msg id equal to last msg id => " << offset;
    TagMap tags;
    StreamingUtility::FindTagsFromQueueName(channel_info.channel_id, tags, true);
    // TODO(lingxuan.zlx): restful api will send many duplicated metric requests
    runtime_context_->ReportMetrics("reader.notify_in_last_message_id", offset, tags);
  }
}

DataReader::DataReader(std::shared_ptr<RuntimeContext> &runtime_context)
    : runtime_context_(runtime_context),
      transfer_config_(std::make_shared<Config>()),
      timer_interval_(0),
      last_bundle_ts_(0),
      last_message_ts_(0),
      last_message_latency_(0),
      last_bundle_latency_(0),
      last_bundle_unit_(0),
      last_getbundle_ts_(0),
      last_getbundle_latency_(0),
      target_global_barrier_cnt_(0) {
  runtime_context_->transfer_state_.SetInit();
}

DataReader::~DataReader() {
  {
    AutoSpinLock lock(transfer_changing_);
    this->Stop();
  }
  runtime_context_->ShutdownTimer();
  if (runtime_context_->config_.GetHttpProfilerEnable()) {
    STREAMING_LOG(INFO) << "Unregister reader.";
    HttpProfiler::GetHttpProfilerInstance()->UnregisterDataReader();
  }
  if (queue_handler_) {
    queue_handler_->Stop();
  }
  STREAMING_LOG(INFO) << "Streaming reader deconstruct.";
}

void DataReader::NotifyConsumed(std::shared_ptr<StreamingReaderBundle> &message) {
  auto &channel_info = channel_info_map_[message->from];
  auto &queue_info = channel_info.queue_info;
  channel_info.notify_cnt++;
  if (queue_info.target_message_id <= message->message_id) {
    // NOTE(lingxuan.zlx): For minimum change in channel api, we reset consumed bundle id
    // in reader notify consumed function so only message offset should be passed in
    // notification in streaming data reader view.
    // To avoid replicated small bundle id, we only mark the latest bundle if for
    // notification.
    if (queue_info.consumed_bundle_id == static_cast<uint64_t>(-1) ||
        queue_info.consumed_bundle_id < message->bundle_id) {
      queue_info.consumed_bundle_id = message->bundle_id;
    }
    STREAMING_LOG(DEBUG) << "Notify consumed bundle id " << message->bundle_id;
    NotifyConsumedItem(channel_info, message->message_id);

    // Count used bytes only this target item is not empty bundle, which will speed up
    // if messages among many empty bundles
    RefreshChannelInfo(channel_info);
    if (queue_info.last_message_id != QUEUE_INVALID_SEQ_ID) {
      uint64_t original_target_message_id = queue_info.target_message_id;
      queue_info.target_message_id = std::min(
          queue_info.last_message_id, message->message_id + step_udpater->GetStep());
      uint64_t target_diff = queue_info.target_message_id - original_target_message_id;
      step_udpater->UpdateStep(target_diff);
      channel_info.last_queue_target_diff = target_diff;
    } else {
      STREAMING_LOG(WARNING) << "[Reader] [QueueInfo] q id " << message->from
                             << ", last msg id " << queue_info.last_message_id;
    }
    STREAMING_LOG(DEBUG) << "[Reader] [Consumed] trigger notify consumed"
                         << ", q_id => " << message->from << ", last msg id => "
                         << queue_info.last_message_id << ", target msg id => "
                         << queue_info.target_message_id << ", consumed msg id => "
                         << message->message_id << ", last message id => "
                         << message->meta->GetLastMessageId() << ", bundle type => "
                         << static_cast<uint32_t>(message->meta->GetBundleType())
                         << ", last message bundle ts => "
                         << message->meta->GetMessageBundleTs();
  }
}

void DataReader::ReportTimer() {
  STREAMING_LOG(INFO) << "Streaming Reader Report, ts => " << current_sys_time_ms();
  TagMap tags;

  for (auto &q_id : input_queue_ids_) {
    StreamingUtility::FindTagsFromQueueName(q_id, tags);
    if (channel_info_map_.find(q_id) == channel_info_map_.end() ||
        channel_map_.find(q_id) == channel_map_.end()) {
      STREAMING_LOG(WARNING) << "Channel id " << q_id << " is unready or invalid";
      continue;
    }
    auto &channel_info = channel_info_map_[q_id];
    runtime_context_->ReportMetrics("reader.message_id", channel_info.current_message_id,
                                    tags);
    STREAMING_LOG(INFO) << " [Reader] [Report] qname => " << tags["qname"]
                        << ", message id => " << channel_info.current_message_id;

    auto &queue_info = channel_info.queue_info;
    runtime_context_->ReportMetrics("reader.first_message_id",
                                    queue_info.first_message_id, tags);
    runtime_context_->ReportMetrics("reader.last_message_id", queue_info.last_message_id,
                                    tags);
    runtime_context_->ReportMetrics("reader.consumed_message_id",
                                    queue_info.consumed_message_id, tags);
    runtime_context_->ReportMetrics("reader.target_message_id",
                                    queue_info.target_message_id, tags);

    runtime_context_->ReportMetrics("reader.last_item_diff",
                                    channel_info.last_queue_item_delay, tags);
    runtime_context_->ReportMetrics("reader.target_message_id_diff",
                                    channel_info.last_queue_target_diff, tags);
    runtime_context_->ReportMetrics("reader.get_item_times",
                                    channel_info.get_queue_item_times, tags);
    runtime_context_->ReportMetrics(
        "reader.timeout_cnt",
        channel_info.get_queue_item_times - channel_info.current_message_id, tags);
    runtime_context_->ReportMetrics("reader.notify_cnt", channel_info.notify_cnt, tags);
    runtime_context_->ReportMetrics("reader.max_notified_msg_id",
                                    channel_info.max_notified_msg_id, tags);

    if (last_read_q_id_ == q_id) {
      runtime_context_->ReportMetrics("reader.last_read_message_id",
                                      channel_info.current_message_id, tags);
      STREAMING_LOG(INFO) << " [Reader] last read message id from " << tags["qname"]
                          << ", message id => " << channel_info.current_message_id;
    }

    std::unordered_map<std::string, double> metrics;
    channel_map_[channel_info.channel_id]->GetMetrics(metrics);
    for (auto &metric : metrics) {
      runtime_context_->ReportMetrics(metric.first, metric.second, tags);
    }
    runtime_context_->ReportMetrics("reader.last_queue_item_read_latency",
                                    channel_info.last_queue_item_read_latency, tags);
    runtime_context_->ReportMetrics(
        "reader.last_queue_item_read_latency_large_than_10ms",
        channel_info.last_queue_item_read_latency_large_than_10ms, tags);
    runtime_context_->ReportMetrics(
        "reader.last_queue_item_read_latency_large_than_5ms",
        channel_info.last_queue_item_read_latency_large_than_5ms, tags);
    runtime_context_->ReportMetrics("reader.last_bundle_merger_latency",
                                    channel_info.last_bundle_merger_latency, tags);
    runtime_context_->ReportMetrics(
        "reader.last_bundle_merger_latency_large_than_10ms",
        channel_info.last_bundle_merger_latency_large_than_10ms, tags);
    runtime_context_->ReportMetrics(
        "reader.last_bundle_merger_latency_large_than_5ms",
        channel_info.last_bundle_merger_latency_large_than_5ms, tags);
  }
  // remove queue name for barrier metric
  tags.erase("qname");
  runtime_context_->ReportMetrics("reader.seq_delay", last_bundle_latency_, tags);
  runtime_context_->ReportMetrics("reader.read_last_message_latency",
                                  last_message_latency_, tags);
  runtime_context_->ReportMetrics("reader.bundle_unit_size", last_bundle_unit_, tags);
  runtime_context_->ReportMetrics("reader.last_getbundle_latency",
                                  last_getbundle_latency_, tags);
}

void DataReader::Stop() { runtime_context_->transfer_state_.SetInterrupted(); }

void DataReader::RefreshChannelInfo(ConsumerChannelInfo &channel_info) {
  channel_map_[channel_info.channel_id]->RefreshChannelInfo();
}

void DataReader::Rescale(
    const std::vector<ObjectID> &id_vec,
    std::vector<StreamingQueueInitialParameter> &init_parameter_vec) {
  /// We assume all upstream nodes will send partial barrier if no given this
  /// parameter.
  Rescale(id_vec, id_vec, init_parameter_vec);
}

void DataReader::Rescale(
    const std::vector<ObjectID> &id_vec, const std::vector<ObjectID> &to_collect_vec,
    std::vector<StreamingQueueInitialParameter> &init_parameter_vec) {
  // We use default collection upstream set (all upstream workers) if newborn
  // worker might not call hotupdate/rescale, but force-update when this
  // function is invoked.
  if (!rescale_context_.update_set_.empty()) {
    STREAMING_LOG(INFO) << "Force clean default barrier collection in rescale update.";
    rescale_context_.update_set_.clear();
  }
  rescale_context_.UpdateRescaleTargetSet(to_collect_vec);

  std::vector<ObjectID> scale_up_ids =
      StreamingUtility::SetDifference(id_vec, input_queue_ids_);
  if (scale_up_ids.size() > 0) {
    STREAMING_LOG(INFO) << "reader scale up";
    // TODO: ugly
    std::vector<StreamingQueueInitialParameter> scale_init_parameter_vec;
    for (auto qid : scale_up_ids) {
      auto it = std::find(id_vec.begin(), id_vec.end(), qid);
      STREAMING_CHECK(it != id_vec.end());
      int index = distance(id_vec.begin(), it);
      scale_init_parameter_vec.push_back(init_parameter_vec[index]);
    }
    ScaleUp(scale_up_ids, scale_init_parameter_vec);
  }
  std::vector<ObjectID> scale_down_ids =
      StreamingUtility::SetDifference(input_queue_ids_, id_vec);

  if (scale_down_ids.size() > 0) {
    rescale_context_.scale_down_removed_ = scale_down_ids;
    // TODO(lingxuan.zlx): To-collect-vec is copy of id vec now, but actually
    // it should be from-barrier-upstream id collection. So this worker will be
    // removed if id vector is empty and it will recieve barriers from all
    // upstream of itself.
    // FIXME: We can pass correct collect-vec to fix it.
    if (rescale_context_.scale_down_removed_.size() == input_queue_ids_.size()) {
      STREAMING_LOG(INFO) << "This worker will be removed, so all upstream nodes "
                          << "should send barrier to it.";
      rescale_context_.update_set_.clear();
      rescale_context_.UpdateRescaleTargetSet(input_queue_ids_);
    }
    decltype(rescale_context_.scale_down_removed_.begin()) ID_type;
    std::function<std::string(decltype(ID_type))> func = [](decltype(ID_type) it) {
      return it->Hex();
    };
    STREAMING_LOG(INFO) << "reader scale down, size "
                        << rescale_context_.scale_down_removed_.size()
                        << StreamingUtility::join(
                               rescale_context_.scale_down_removed_.begin(),
                               rescale_context_.scale_down_removed_.end(), func, ",",
                               "|");
  }
}

StreamingStatus DataReader::DestroyChannel(const std::vector<ObjectID> &ids) {
  for (auto &id : ids) {
    STREAMING_LOG(INFO) << "remove from reader => " << id;
    input_queue_ids_.erase(
        std::remove_if(input_queue_ids_.begin(), input_queue_ids_.end(),
                       [&id](const ObjectID &input_id) { return input_id == id; }));
    // This is no initialization of reader_merger if it's AT_LEAST_ONCE.
    if (reader_merger_) {
      reader_merger_->remove(id);
    }
    auto channel_info_it = channel_info_map_.find(id);
    if (channel_info_it == channel_info_map_.end()) {
      STREAMING_LOG(WARNING)
          << "The channel is not found in map, skip it while destroying " << id;
      continue;
    }
    channel_map_[channel_info_it->second.channel_id]->DestroyTransferChannel();
    channel_info_map_.erase(channel_info_it);
    auto reset_cached_item = [&id](std::shared_ptr<StreamingReaderBundle> &bundle_item) {
      if (bundle_item && bundle_item->from == id) {
        STREAMING_LOG(INFO) << "last fetched queue item reset";
        bundle_item.reset();
      }
    };
    reset_cached_item(last_fetched_queue_item_);
    reset_cached_item(reader_cache_item_memory_);
  }
  target_global_barrier_cnt_ = GetTargetBarrierCount(input_queue_ids_);
  // Reset channel info map iterator for round-robin fetching.
  channel_info_map_iter_ = channel_info_map_.begin();
  return StreamingStatus::OK;
}

void DataReader::ClearPartialCheckpoint(uint64_t global_barrier_id,
                                        uint64_t partial_barrier_id) {
  STREAMING_LOG(INFO) << "[Reader][PartialBarrier]  clear paritial checkpoint, "
                      << " global barrier id => " << global_barrier_id
                      << ", partial barrier id => " << partial_barrier_id;
  auto &scale_down_set = rescale_context_.scale_down_removed_;
  if (scale_down_set.size()) {
    ScaleDown(scale_down_set);
    scale_down_set.clear();
  }
  rescale_context_.ClearContext();
}
void DataReader::ScaleUp(
    const std::vector<ray::ObjectID> &id_vec,
    std::vector<StreamingQueueInitialParameter> &scale_init_parameter_vec) {
  StreamingRescaleRAII auto_update(this);
  uint64_t min_barrier_id = static_cast<uint64_t>(-1);
  uint64_t min_partial_barrier_id = static_cast<uint64_t>(-1);
  for (auto &offset_item : channel_info_map_) {
    if (min_barrier_id > offset_item.second.barrier_id) {
      min_barrier_id = offset_item.second.barrier_id;
    }
    if (min_partial_barrier_id > offset_item.second.partial_barrier_id) {
      min_partial_barrier_id = offset_item.second.partial_barrier_id;
    }
  }
  for (unsigned int i = 0; i < id_vec.size(); i++) {
    auto &id = id_vec[i];
    if (channel_info_map_.find(id) != channel_info_map_.end()) {
      STREAMING_LOG(WARNING)
          << "The channel is already in map when scale up, so skipt it " << id;
      continue;
    }
    channel_info_map_[id].current_message_id = 0;
    channel_info_map_[id].barrier_id = min_barrier_id;
    channel_info_map_[id].partial_barrier_id = min_partial_barrier_id;
    channel_info_map_[id].parameter = scale_init_parameter_vec[i];
    rescale_context_.scale_up_added_.push_back(id);
  }
  Init(rescale_context_.scale_up_added_, scale_init_parameter_vec, timer_interval_, true);
}

void DataReader::ScaleDown(const std::vector<ray::ObjectID> &id_vec) {
  StreamingRescaleRAII auto_update(this);
  DestroyChannel(id_vec);
}

void DataReader::AdvanceChannelInfoMapIter() {
  ++channel_info_map_iter_;
  if (channel_info_map_iter_ == channel_info_map_.end()) {
    channel_info_map_iter_ = channel_info_map_.begin();
  }
}

void DataReader::OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
  if (queue_handler_) {
    queue_handler_->DispatchMessageAsync(buffers);
  } else {
    STREAMING_LOG(ERROR) << "Downstream handler not ready.";
  }
}

std::shared_ptr<LocalMemoryBuffer> DataReader::OnMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  if (queue_handler_) {
    return queue_handler_->DispatchMessageSync(buffer);
  } else {
    STREAMING_LOG(ERROR) << "Downstream handler not ready.";
    return nullptr;
  }
}

void DataReader::SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) {
  queue_handler_ = handler;
}

void DataReader::NotifyStartLogging(uint64_t barrier_id) {
  for (auto &value : channel_info_map_) {
    auto &channel_info = value.second;
    if (channel_info.parameter.cyclic) {
      channel_map_[channel_info.channel_id]->NotifyStartLogging(
          barrier_id, channel_info.current_message_id + 1, 0);
    }
  }
}

std::string DataReader::GetProfilingInfo() const {
  if (runtime_context_->config_.GetHttpProfilerEnable()) {
    rapidjson::Document doc(rapidjson::kObjectType);
    HttpProfiler::GetHttpProfilerInstance()->DumpConsumerInfo(doc);
    return rapidjson::to_string(doc);
  } else {
    return std::string("");
  }
}

bool StreamingReaderMsgPtrComparator::operator()(
    const std::shared_ptr<StreamingReaderBundle> &a,
    const std::shared_ptr<StreamingReaderBundle> &b) {
  if (comp_strategy == ReliabilityLevel::EXACTLY_ONCE && !a->cyclic && !b->cyclic) {
    if (a->last_barrier_id != b->last_barrier_id)
      return a->last_barrier_id > b->last_barrier_id;

    // NOTE: Currently, Checkpoint and UDC controller can not both in the data stream!
    if (a->last_udc_barrier_id != b->last_udc_barrier_id)
      return a->last_udc_barrier_id > b->last_udc_barrier_id;
  }
  STREAMING_CHECK(a->meta);
  // we proposed push id for stability of message in sorting
  if (a->meta->GetMessageBundleTs() == b->meta->GetMessageBundleTs()) {
    return a->from.Hash() > b->from.Hash();
  }
  return a->meta->GetMessageBundleTs() > b->meta->GetMessageBundleTs();
}

int32_t DataReader::GetTargetBarrierCount(const std::vector<ObjectID> &queue_ids) {
  return std::count_if(queue_ids.begin(), queue_ids.end(),
                       [this](const ObjectID &queue_id) {
                         return !channel_info_map_[queue_id].parameter.cyclic;
                       });
}

const StreamingConfig &DataReader::GetConfig() const {
  return runtime_context_->GetConfig();
}

}  // namespace streaming
}  // namespace ray
